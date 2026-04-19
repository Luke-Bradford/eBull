import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { LayerEntry } from "@/api/types";

import { LayerHealthList } from "./LayerHealthList";


function mk(overrides: Partial<LayerEntry>): LayerEntry {
  return {
    layer: overrides.layer ?? "universe",
    display_name: overrides.display_name ?? "Tradable Universe",
    state: overrides.state ?? "healthy",
    last_updated: overrides.last_updated ?? null,
    plain_language_sla: overrides.plain_language_sla ?? "Refreshed weekly.",
  };
}


describe("LayerHealthList", () => {
  it("renders one row per layer", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "universe", display_name: "Tradable Universe" }),
      mk({ layer: "candles", display_name: "Daily Price Candles" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText("Tradable Universe")).toBeInTheDocument();
    expect(screen.getByText("Daily Price Candles")).toBeInTheDocument();
  });

  it("renders healthy pill as Healthy", () => {
    const layers: LayerEntry[] = [mk({ layer: "universe", state: "healthy" })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByLabelText("universe state")).toHaveTextContent(/healthy/i);
  });

  it("renders disabled pill as Disabled", () => {
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "disabled" })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByLabelText("candles state")).toHaveTextContent(/disabled/i);
  });

  it("renders action_needed pill as Needs attention", () => {
    const layers: LayerEntry[] = [mk({ layer: "cik_mapping", state: "action_needed" })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByLabelText("cik_mapping state")).toHaveTextContent(/needs attention/i);
  });

  it("renders secret_missing pill as Needs attention", () => {
    const layers: LayerEntry[] = [mk({ layer: "news", state: "secret_missing" })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByLabelText("news state")).toHaveTextContent(/needs attention/i);
  });

  it("renders running / retrying / cascade_waiting / degraded as Catching up", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "r1", state: "running" }),
      mk({ layer: "r2", state: "retrying" }),
      mk({ layer: "r3", state: "cascade_waiting" }),
      mk({ layer: "r4", state: "degraded" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    for (const name of ["r1", "r2", "r3", "r4"]) {
      expect(screen.getByLabelText(`${name} state`)).toHaveTextContent(/catching up/i);
    }
  });

  it("renders relative last_updated when present", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-19T12:00:00Z"));
    try {
      const layers: LayerEntry[] = [
        mk({ layer: "universe", last_updated: "2026-04-19T11:00:00Z" }),
      ];
      render(<LayerHealthList layers={layers} onToggle={() => {}} />);
      expect(screen.getByText(/1h ago/i)).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });

  it("renders 'just now' when last_updated is less than a minute ago", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-19T12:00:00Z"));
    try {
      const layers: LayerEntry[] = [
        mk({ layer: "universe", last_updated: "2026-04-19T11:59:45Z" }),
      ];
      render(<LayerHealthList layers={layers} onToggle={() => {}} />);
      expect(screen.getByText(/just now/i)).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });

  it("renders 'never' when last_updated is null", () => {
    const layers: LayerEntry[] = [mk({ layer: "universe", last_updated: null })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText(/never/i)).toBeInTheDocument();
  });

  it("renders SLA below the row", () => {
    const layers: LayerEntry[] = [mk({ layer: "candles", plain_language_sla: "Refreshed after market close." })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText(/refreshed after market close/i)).toBeInTheDocument();
  });

  it("mounts each row with id=admin-layer-<name> for deep-linking", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "cik_mapping", display_name: "SEC CIK Mapping" }),
    ];
    const { container } = render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(container.querySelector("#admin-layer-cik_mapping")).not.toBeNull();
  });

  it("toggle button fires onToggle with opposite enabled state", () => {
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "healthy" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("candles actions"));
    fireEvent.click(screen.getByText(/Disable layer/));
    expect(onToggle).toHaveBeenCalledWith("candles", false);
  });

  it("re-enables a disabled layer via the menu", () => {
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "disabled" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("candles actions"));
    fireEvent.click(screen.getByText(/Enable layer/));
    expect(onToggle).toHaveBeenCalledWith("candles", true);
  });

  it("closes the open menu when clicking outside", () => {
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "healthy" })];
    render(
      <div>
        <LayerHealthList layers={layers} onToggle={onToggle} />
        <div data-testid="outside">outside content</div>
      </div>
    );
    fireEvent.click(screen.getByLabelText("candles actions"));
    expect(screen.getByText(/Disable layer/)).toBeInTheDocument();
    fireEvent.mouseDown(screen.getByTestId("outside"));
    expect(screen.queryByText(/Disable layer/)).toBeNull();
  });

  it("closes the open menu on Escape key", () => {
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "healthy" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("candles actions"));
    expect(screen.getByText(/Disable layer/)).toBeInTheDocument();
    fireEvent.keyDown(document, { key: "Escape" });
    expect(screen.queryByText(/Disable layer/)).toBeNull();
  });

  it("opening a second row's menu closes the first", () => {
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [
      mk({ layer: "candles", state: "healthy" }),
      mk({ layer: "universe", state: "healthy" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("candles actions"));
    // Both "Disable layer" buttons render the same text, so count menu items.
    expect(screen.getAllByText(/Disable layer/)).toHaveLength(1);
    fireEvent.click(screen.getByLabelText("universe actions"));
    // First menu is closed; second is open; total remains exactly 1 dropdown.
    expect(screen.getAllByText(/Disable layer/)).toHaveLength(1);
  });
});

describe("LayerHealthList safety-critical confirm", () => {
  it("prompts window.confirm when disabling fx_rates", () => {
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [
      mk({ layer: "fx_rates", state: "healthy", display_name: "FX Rates" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("fx_rates actions"));
    fireEvent.click(screen.getByText(/Disable layer/));
    expect(confirmSpy).toHaveBeenCalled();
    expect(onToggle).toHaveBeenCalledWith("fx_rates", false);
    confirmSpy.mockRestore();
  });

  it("does not call onToggle when safety-critical confirm is declined", () => {
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(false);
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [
      mk({ layer: "portfolio_sync", state: "healthy", display_name: "Portfolio Sync" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("portfolio_sync actions"));
    fireEvent.click(screen.getByText(/Disable layer/));
    expect(onToggle).not.toHaveBeenCalled();
    confirmSpy.mockRestore();
  });

  it("does not prompt confirm for non-safety-critical layers", () => {
    const confirmSpy = vi.spyOn(window, "confirm");
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "healthy" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("candles actions"));
    fireEvent.click(screen.getByText(/Disable layer/));
    expect(confirmSpy).not.toHaveBeenCalled();
    expect(onToggle).toHaveBeenCalledWith("candles", false);
    confirmSpy.mockRestore();
  });

  it("skips confirm when re-enabling a safety-critical layer", () => {
    const confirmSpy = vi.spyOn(window, "confirm");
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "fx_rates", state: "disabled" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("fx_rates actions"));
    fireEvent.click(screen.getByText(/Enable layer/));
    expect(confirmSpy).not.toHaveBeenCalled();
    expect(onToggle).toHaveBeenCalledWith("fx_rates", true);
    confirmSpy.mockRestore();
  });
});
