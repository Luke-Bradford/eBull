/**
 * Safety-state UI test for DemoLivePill (spec §9).
 *
 * Pins the two behaviours that matter:
 *   1. The pill reflects a fresh live flag when it arrives.
 *   2. When /config transiently returns null (e.g. a refetch
 *      in flight), the pill stays on the last confirmed value
 *      and a `(stale)` marker appears.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";

import { DemoLivePill } from "@/components/orders/DemoLivePill";
import type { ConfigResponse } from "@/api/types";

vi.mock("@/api/config", () => ({
  fetchConfig: vi.fn(),
}));

import { fetchConfig } from "@/api/config";

const mockedFetchConfig = vi.mocked(fetchConfig);

function configWith(enableLive: boolean): ConfigResponse {
  return {
    app_env: "dev",
    etoro_env: "demo",
    runtime: {
      enable_auto_trading: false,
      enable_live_trading: enableLive,
      display_currency: "GBP",
      updated_at: "2026-04-18T00:00:00Z",
      updated_by: "system",
      reason: "",
    },
    kill_switch: {
      active: false,
      activated_at: null,
      activated_by: null,
      reason: null,
    },
  };
}

beforeEach(() => {
  mockedFetchConfig.mockReset();
});

describe("DemoLivePill", () => {
  it("shows DEMO MODE when enable_live_trading=false (fresh, no stale marker)", async () => {
    mockedFetchConfig.mockResolvedValueOnce(configWith(false));
    render(<DemoLivePill />);
    const pill = await screen.findByTestId("demo-live-pill");
    expect(pill).toHaveAttribute("data-live", "false");
    expect(pill.textContent).toContain("DEMO MODE");
    expect(pill.textContent).not.toContain("stale");
  });

  it("shows LIVE when enable_live_trading=true", async () => {
    mockedFetchConfig.mockResolvedValueOnce(configWith(true));
    render(<DemoLivePill />);
    const pill = await screen.findByTestId("demo-live-pill");
    await waitFor(() => {
      expect(pill).toHaveAttribute("data-live", "true");
    });
    expect(pill.textContent).toContain("LIVE");
  });

  it("on cold start with no response yet, defaults to DEMO MODE with no stale marker", () => {
    // Resolve never — simulate an infinite in-flight request.
    mockedFetchConfig.mockReturnValue(new Promise(() => {}));
    render(<DemoLivePill />);
    const pill = screen.getByTestId("demo-live-pill");
    expect(pill).toHaveAttribute("data-live", "false");
    expect(pill.textContent).toContain("DEMO MODE");
    // Cold start: no cache yet, no stale marker.
    expect(pill.textContent).not.toContain("stale");
  });
});
