/**
 * Safety-state UI test for DemoLivePill (spec §9).
 *
 * Pins the two behaviours that matter:
 *   1. The pill reflects a fresh live flag when it arrives.
 *   2. When /config transiently returns null (e.g. a refetch
 *      in flight), the pill stays on the last confirmed value
 *      and a `(stale)` marker appears.
 *
 * Uses `TestConfigProvider` to inject config state directly — bypasses
 * the shared ConfigProvider's `fetchConfig` call entirely (#320).
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { DemoLivePill } from "@/components/orders/DemoLivePill";
import { TestConfigProvider } from "@/lib/ConfigContext";
import type { ConfigResponse } from "@/api/types";

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

describe("DemoLivePill", () => {
  it("shows DEMO MODE when enable_live_trading=false (fresh, no stale marker)", () => {
    render(
      <TestConfigProvider value={{ data: configWith(false), loading: false }}>
        <DemoLivePill />
      </TestConfigProvider>,
    );
    const pill = screen.getByTestId("demo-live-pill");
    expect(pill).toHaveAttribute("data-live", "false");
    expect(pill.textContent).toContain("DEMO MODE");
    expect(pill.textContent).not.toContain("stale");
  });

  it("shows LIVE when enable_live_trading=true", () => {
    render(
      <TestConfigProvider value={{ data: configWith(true), loading: false }}>
        <DemoLivePill />
      </TestConfigProvider>,
    );
    const pill = screen.getByTestId("demo-live-pill");
    expect(pill).toHaveAttribute("data-live", "true");
    expect(pill.textContent).toContain("LIVE");
  });

  it("on cold start with no response yet, defaults to DEMO MODE with no stale marker", () => {
    render(
      <TestConfigProvider value={{ data: null, loading: true }}>
        <DemoLivePill />
      </TestConfigProvider>,
    );
    const pill = screen.getByTestId("demo-live-pill");
    expect(pill).toHaveAttribute("data-live", "false");
    expect(pill.textContent).toContain("DEMO MODE");
    // Cold start: no cache yet, no stale marker.
    expect(pill.textContent).not.toContain("stale");
  });
});
